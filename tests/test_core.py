import asyncio
import logging
import os
from typing import List

import pytest
import pytest_asyncio

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
        pass
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


def ASYNC_TEST(fn):
    return pytest.mark.timeout(TIMEOUT)(pytest.mark.asyncio(fn))


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
