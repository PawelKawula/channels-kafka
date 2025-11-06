import asyncio
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
MAX_SIZE = int(os.getenv("CHANNELS_KAFKA_TEST_MAX_SIZE", 100))
TIMEOUT = int(os.getenv("CHANNELS_KAFKA_TEST_TIMEOUT", 15))

logger = logging.getLogger(__name__)


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
        MAX_SIZE,
    )
    yield None
    await layer.flush()
    await asyncio.sleep(5)
    await layer.close()


@pytest_asyncio.fixture
async def layer():
    layer = None
    try:
        layer = core.KafkaChannelLayer(
            BOOTSTRAP_SERVERS,
            CLIENT_ID,
            GROUP_ID,
            TOPIC_ID,
            MAX_SIZE,
        )
        yield layer
    finally:
        if layer:
            try:
                async with asyncio.timeout(TIMEOUT):
                    await layer.flush()
                    await layer.close()
            except:
                logger.error(
                    "Couldn't delete test topic %s or it wasn't even created", TOPIC_ID
                )
                raise


def ASYNC_TEST(fn, timeout=None):
    return pytest.mark.timeout(timeout or TIMEOUT)(pytest.mark.asyncio(fn))


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


@pytest.mark.skip
@ASYNC_TEST
async def test_send_expire_remotely(layer: core.KafkaChannelLayer):
    # expiry 80ms: long enough for us to receive all messages; short enough to
    # keep the test fast.
    await layer.send("x!y", {"type": "test.message1"})  # one local, unacked
    await layer.send("x!y", {"type": "test.message2"})  # remote, queued
    await asyncio.sleep(0.09)  # test.message2 should expire
    await layer.send("x!y", {"type": "test.message3"})  # remote
    assert (await layer.receive("x!y"))["type"] == "test.message1"
    # test.message2 should disappear entirely
    assert (await layer.receive("x!y"))["type"] == "test.message3"


# @pytest.mark.xfail(reason="Batching in kafka")
@ASYNC_TEST
async def test_send_expire_locally(layer: core.KafkaChannelLayer, caplog):
    # expiry 20ms: long enough that we can deliver one message but expire
    # another.
    #
    # local_capacity=1: when we expire, we must ack so RabbitMQ can send
    # another message.
    await layer.send("x!y", {"type": "test.message1"})
    logger.warning("sleeping...")
    await asyncio.sleep(10)  # plenty of time; message.1 should expire
    logger.warning("slept, should be expired")
    await layer.send("x!y", {"type": "test.message2"})
    logger.warning("sent second")
    # assert (await layer.receive("x!y"))["type"] == "test.message1"
    assert (await layer.receive("x!y"))["type"] == "test.message2"
    assert "expired locally" in caplog.text


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
