from typing import Callable, Optional

from common.comms.messages import Message, MessageType

from .checkpointer import Checkpointer


def dispatch(
    checkpointer: Optional[Checkpointer],
    msg: Message,
    ack: Callable,
    on_eof: Callable[[Message], None],
    on_data: Callable[[Message], None],
):
    """Route a data message through dedup + batched checkpointing, or flush the
    checkpoint on EOF. Shared by every checkpointed controller; `on_eof`/`on_data`
    hold the controller-specific processing + emit."""
    if msg.type() == MessageType.EOF:
        if checkpointer is not None:
            checkpointer.flush()
        on_eof(msg)
        ack()
        return

    if checkpointer is None:
        on_data(msg)
        ack()
        return

    checkpointer.handle_data(msg, lambda: on_data(msg), ack)
