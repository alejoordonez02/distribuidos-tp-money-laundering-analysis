from typing import Callable, Optional

from common.comms.messages import Message, MessageType
from common.fault_injection import maybe_crash

from .checkpointer import Checkpointer


def dispatch(
    checkpointer: Optional[Checkpointer],
    msg: Message,
    ack: Callable,
    on_eof: Callable[[Message], None],
    on_data: Callable[[Message], None],
    on_abort: Optional[Callable[[Message], None]] = None,
):
    """Route a data message through dedup + batched checkpointing, or flush the
    checkpoint on EOF. Shared by every checkpointed controller; `on_eof`/`on_data`
    hold the controller-specific processing + emit. `on_abort` drops a crashed
    client's partial state when an Abort arrives."""
    if msg.type() == MessageType.EOF:
        maybe_crash("before_eof_flush")
        if checkpointer is not None:
            checkpointer.flush()
        maybe_crash("after_eof_flush_before_handle")
        on_eof(msg)
        maybe_crash("after_eof_handle_before_ack")
        ack()
        return

    if msg.type() == MessageType.ABORT:
        if on_abort is not None:
            on_abort(msg)
        ack()
        return

    def run():
        on_data(msg)

    if checkpointer is None:
        run()
        ack()
        return

    checkpointer.handle_data(msg, run, ack)
