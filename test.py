import asyncio

from channels_kafka.core import KafkaChannelLayer


async def main():
    kcl = KafkaChannelLayer(["localhost:9094"])
    await kcl.send("test", {"message": "test msg"})
    msg = await kcl.receive("test")
    print(msg)


if __name__ == "__main__":
    asyncio.run(main())
