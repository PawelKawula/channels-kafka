from typing import Any, Dict, NamedTuple, Union


class ChannelRecipient(NamedTuple):
    channel: str


class GroupRecipient(NamedTuple):
    group: str


Recipient = Union[ChannelRecipient, GroupRecipient]


class DeserializeResult(NamedTuple):
    recipient: Recipient
    data: Dict[str, Any]


def serialize_message(recipient: Recipient, message: dict):
    assert "__asgi_channel__" not in message
    assert "__asgi_group__" not in message
    augmented_message = dict(message)
    if isinstance(recipient, ChannelRecipient):
        augmented_message["__asgi_channel__"] = recipient.channel
    elif isinstance(recipient, GroupRecipient):
        augmented_message["__asgi_group__"] = recipient.group
    return augmented_message


def deserialize_message(message: dict):
    group = message.pop("__asgi_group__", None)
    channel = message.pop("__asgi_channel__", None)
    assert sum(map(bool, (channel, group))) == 1
    if channel is not None:
        recipient = ChannelRecipient(channel)
    else:
        recipient = GroupRecipient(group)
    return DeserializeResult(recipient, message)
