import asyncio
import contextlib
import logging
import os
import threading
from typing import List

import pytest
import pytest_asyncio
from channels.exceptions import ChannelFull

from channels_kafka import core

BOOTSTRAP_SERVERS: List[str] = os.getenv(
    "CHANNELS_KAFKA_TEST_BOOTSTRAP_SERVERS", "localhost:9092"
).split(",")
CLIENT_ID = os.getenv("CHANNELS_KAFKA_TEST_CLIENT_ID", "channels_kafka_testclient")
GROUP_ID = os.getenv("CHANNELS_KAFKA_TEST_GROUP_ID", "channels_kafka_testgroup")
TOPIC_ID = os.getenv("CHANNELS_KAFKA_TEST_TOPIC", "channels_kafka_testtopic")
LOCAL_CAPACITY = int(os.getenv("CHANNELS_KAFKA_LOCAL_CAPACITY", 100))
LOCAL_EXPIRY = int(os.getenv("CHANNELS_KAFKA_LOCAL_EXPIRY", 1))
LOG_RETENTION = float(os.getenv("CHANNELS_LOG_RETENTION", 10))
TEST_TIMEOUT = int(os.getenv("CHANNELS_KAFKA_TEST_TIMEOUT", 15))

logger = logging.getLogger(__name__)


async def wait_for_output_in_logging(caplog, output, timeout=3):
    async with asyncio.timeout(timeout):
        while True:
            if output in caplog.text:
                return
            await asyncio.sleep(0.5)


@pytest.fixture
def asyncio_default_fixture_loop_scope():
    return "session"


@pytest.fixture(scope="session")
async def flush_channel_layer(layer):
    layer = core.KafkaChannelLayer(
        BOOTSTRAP_SERVERS,
        CLIENT_ID,
        GROUP_ID,
        TOPIC_ID,
        LOCAL_CAPACITY,
    )
    yield None
    await layer.flush()
    await asyncio.sleep(5)
    await layer.close()


async def layer(**kwargs):
    layer = None
    try:
        default_kwargs = {
            "hosts": BOOTSTRAP_SERVERS,
            "client_id": CLIENT_ID,
            "group_id": GROUP_ID,
            "topic": TOPIC_ID,
            "local_capacity": LOCAL_CAPACITY,
            "local_expiry": LOCAL_EXPIRY,
            "timeout": LOG_RETENTION,
        }
        kwargs = default_kwargs | kwargs
        layer = core.KafkaChannelLayer(**kwargs)
        yield layer
    finally:
        if layer:
            try:
                async with asyncio.timeout(TEST_TIMEOUT):
                    await layer.flush()
                    await layer.close()
            except:
                logger.error(
                    "Couldn't delete test topic %s or it wasn't even created", TOPIC_ID
                )
                raise


open_layer = contextlib.asynccontextmanager(layer)
layer = pytest_asyncio.fixture(layer)


def ASYNC_TEST(fn, timeout=None):
    return pytest.mark.timeout(timeout or TEST_TIMEOUT)(pytest.mark.asyncio(fn))


@ASYNC_TEST
async def test_send_receive(layer: core.KafkaChannelLayer):
    msg = {"message": "test"}
    channel = "testchannel"
    await layer.send(channel, msg)
    received_msg = await layer.receive(channel)
    assert received_msg == msg


@ASYNC_TEST
async def test_send_group(layer: core.KafkaChannelLayer):
    msg = {"message": "group message"}
    group = "testgroup"
    channels = ("channel1", "channel2", "channel3")
    for channel in channels:
        await layer.group_add(group, channel)
    await layer.group_send(group, msg)
    await asyncio.gather(*(layer.receive(ch) for ch in channels))


@ASYNC_TEST
async def test_multiple_event_loops(layer: core.KafkaChannelLayer):
    """
    Makes sure we can receive from two different event loops using
    process-local channel names.

    Real-world callers shouldn't be creating an excessive number of event
    loops. This test is mostly useful for unit-testers and people who use
    async_to_sync() to send messages.
    """
    channel = await layer.new_channel()

    def run():
        with pytest.raises(RuntimeError) as cm:
            asyncio.run(
                layer.send(channel, {"type": "test.message", "text": "Ahoy-hoy!"})
            )
        assert (
            "The caller tried using channels_rabbitmq on a different event loop"
            in str(cm.value)
        )

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    thread.join()


@ASYNC_TEST
async def test_process_local_send_receive(layer: core.KafkaChannelLayer):
    """
    Makes sure we can send a message to a process-local channel then receive it.
    """
    channel = await layer.new_channel()
    await layer.send(channel, {"type": "test.message", "text": "Local only please"})
    message = await layer.receive(channel)
    assert message["type"] == "test.message"
    assert message["text"] == "Local only please"


@ASYNC_TEST
async def test_reject_bad_channel(layer: core.KafkaChannelLayer):
    """
    Makes sure sending/receiving on an invalid channel name fails.
    """
    with pytest.raises(TypeError):
        await layer.send("=+135!", {"type": "foom"})
    with pytest.raises(TypeError):
        await layer.receive("=+135!")


@ASYNC_TEST
async def test_groups_within_layer(layer: core.KafkaChannelLayer):
    """
    Tests basic group operation.
    """
    channel1 = await layer.new_channel()
    channel2 = await layer.new_channel()
    channel3 = await layer.new_channel()
    await layer.group_add("test-group", channel1)
    await layer.group_add("test-group", channel2)
    await layer.group_add("test-group", channel3)
    await layer.group_discard("test-group", channel2)
    await layer.group_send("test-group", {"type": "message.1"})

    # Make sure we get the message on the two channels that were in
    assert (await layer.receive(channel1))["type"] == "message.1"
    assert (await layer.receive(channel3))["type"] == "message.1"

    # channel2 is unsubscribed. It should receive _other_ messages, though.
    await layer.send(channel2, {"type": "message.2"})
    assert (await layer.receive(channel2))["type"] == "message.2"


def test_async_to_sync_without_event_loop():
    with pytest.raises(RuntimeError) as cm:
        _ = core.KafkaChannelLayer()

    assert "Refusing to initialize channel layer without a running event loop" in str(
        cm.value
    )


@pytest.mark.skip
@ASYNC_TEST
async def test_send_capacity(layer: core.KafkaChannelLayer, caplog):
    """
    Makes sure we get ChannelFull when the queue exceeds remote_capacity
    """
    for _ in range(100):
        await layer.send("x!y", {"type": "test.message1"})  # one local, unacked
    await layer.send("x!y", {"type": "test.message2"})  # one remote, queued
    with pytest.raises(ChannelFull):
        await layer.send("x!y", {"type": "test.message3"})
    assert "Back-pressuring. Biggest queues: x!y (1)" in caplog.text

    # Test that even after error, the queue works as expected.

    # Receive the acked message1. This will _eventually_ ack message2. RabbitMQ
    # will have unacked=0, ready=1. This will prompt it to send a new unacked
    # message.
    assert (await layer.receive("x!y"))["type"] == "test.message1"

    # Receive message2. This _guarantees_ message2 is acked.
    assert (await layer.receive("x!y"))["type"] == "test.message2"

    # Send message5. We're sending and receiving on the same TCP layer, so
    # RabbitMQ is aware that message2 was acked by the time we send message5.
    # That means its queue isn't maxed out any more.
    await layer.send("x!y", {"type": "test.message4"})  # one ready

    assert (await layer.receive("x!y"))["type"] == "test.message4"


@ASYNC_TEST
async def test_send_expire_remotely(layer: core.KafkaChannelLayer):
    async with open_layer(local_capacity=1, local_expiry=LOG_RETENTION * 2) as layer:
        await layer.send("x!y", {"type": "test.message1"})  # one local, unacked
        await layer.send("x!y", {"type": "test.message2"})  # remote, queued
        assert (await layer.receive("x!y"))["type"] == "test.message1"
        await layer.send("x!y", {"type": "test.message3"})  # remote
        assert (await layer.receive("x!y"))["type"] == "test.message3"


@ASYNC_TEST
async def test_send_expire_locally(layer: core.KafkaChannelLayer, caplog):
    await layer.send("x!y", {"type": "test.message1"})
    await wait_for_output_in_logging(caplog, "expired locally", 10)
    await layer.send("x!y", {"type": "test.message2"})
    assert (await layer.receive("x!y"))["type"] == "test.message2"


@ASYNC_TEST
async def test_multi_send_receive(layer: core.KafkaChannelLayer):
    """
    Tests overlapping sends and receives, and ordering.
    """
    await layer.send("x!y", {"type": "message.1"})
    await layer.send("x!y", {"type": "message.2"})
    await layer.send("x!y", {"type": "message.3"})
    assert (await layer.receive("x!y"))["type"] == "message.1"
    assert (await layer.receive("x!y"))["type"] == "message.2"
    assert (await layer.receive("x!y"))["type"] == "message.3"


@ASYNC_TEST
async def test_groups_local(layer: core.KafkaChannelLayer):
    await layer.group_add("test-group", "x!1")
    await layer.group_add("test-group", "x!2")
    await layer.group_add("test-group", "x!3")
    await layer.group_discard("test-group", "x!2")
    await layer.group_send("test-group", {"type": "message.1"})

    # Make sure we get the message on the two channels that were in
    assert (await layer.receive("x!1"))["type"] == "message.1"
    assert (await layer.receive("x!3"))["type"] == "message.1"

    # "x!2" is unsubscribed. It should receive _other_ messages, though.
    await layer.send("x!2", {"type": "message.2"})
    assert (await layer.receive("x!2"))["type"] == "message.2"


@ASYNC_TEST
async def test_groups_discard(layer: core.KafkaChannelLayer):
    await layer.group_add("test-group", "x!1")
    await layer.group_discard("test-group", "x!1")
    await layer.group_add("test-group", "x!1")
    await layer.group_discard("test-group", "x!1")
    await layer.group_send("test-group", {"type": "ignored"})

    # message was ignored. We should receive _other_ messages, though.
    await layer.send("x!1", {"type": "normal"})
    assert (await layer.receive("x!1"))["type"] == "normal"


@ASYNC_TEST
async def test_group_discard_when_not_connected(layer: core.KafkaChannelLayer):
    await layer.group_discard("test-group", "x!1")
    await layer.group_send("test-group", {"type": "ignored"})
    await layer.send("x!1", {"type": "normal"})
    assert (await layer.receive("x!1"))["type"] == "normal"
