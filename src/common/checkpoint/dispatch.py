from typing import Callable, Optional

from common.comms.messages import Message, MessageType
from common.comms.middleware import InputContext
from common.fault_injection import maybe_crash

from .checkpointer import Checkpointer


def dispatch(
    checkpointer: Optional[Checkpointer],
    msg: Message,
    ack: Callable,
    on_eof: Callable[[Message], None],
    on_data: Callable[[Message], None],
    input_ctx: Optional[InputContext] = None,
):
    """Route a data message through dedup + batched checkpointing, or flush the
    checkpoint on EOF. Shared by every checkpointed controller; `on_eof`/`on_data`
    hold the controller-specific processing + emit. When `input_ctx` is set (a
    competing-consumer node) the input identity is published before each emit, so
    outputs get stamped with an id derived from it."""
    if msg.type() == MessageType.EOF:
        maybe_crash("before_eof_flush")
        if checkpointer is not None:
            checkpointer.flush()
        maybe_crash("after_eof_flush_before_handle")
        on_eof(msg)
        maybe_crash("after_eof_handle_before_ack")
        ack()
        return

    def run():
        if input_ctx is not None:
            input_ctx.set_input(msg.producer_id, msg.seq)
        on_data(msg)

    if checkpointer is None:
        run()
        ack()
        return

    checkpointer.handle_data(msg, run, ack)
