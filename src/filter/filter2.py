import logging
from dataclasses import dataclass
from threading import Lock, Thread
from typing import Callable
from uuid import UUID

from filter_fns import FilterFn

from common.comms.messages import EOF, MessageType, deserialize_message
from common.comms.middleware import MessageMiddlewareQueue


@dataclass
class _ClientState:
    processed_count: int = 0
    is_initiator: bool = False
    done: bool = False

    def share_count(self) -> int:
        """Returns processed_count and resets it to 0."""
        count = self.processed_count
        self.processed_count = 0
        return count


class Filter:
    def __init__(
        self,
        worker_id: int,
        messages_rx: MessageMiddlewareQueue,
        ring_rx: MessageMiddlewareQueue,
        ring_tx: MessageMiddlewareQueue,
        routes: list[tuple[MessageMiddlewareQueue, FilterFn]],
    ):
        self.worker_id = worker_id
        self.messages_rx = messages_rx
        self.ring_rx = ring_rx
        self.ring_tx = ring_tx
        self.routes = routes
        self._state: dict[UUID, _ClientState] = {}
        self._lock = Lock()

    def start(self):
        t = Thread(target=self._ring_thread, daemon=True)
        t.start()
        self.messages_rx.start_consuming(self._handle_message)

    def _ring_thread(self):
        try:
            self.ring_rx.start_consuming(self._handle_ring_eof)
        except Exception as e:
            logging.error(f"ring thread crashed: {e}", exc_info=True)

    def _get_state(self, client_id: UUID) -> _ClientState:
        if client_id not in self._state:
            self._state[client_id] = _ClientState()
        return self._state[client_id]

    def _handle_message(self, bytes2: bytes, ack: Callable, nack: Callable):
        msg = deserialize_message(bytes2)
        logging.debug(f"received msg: {msg.__dict__}")

        if msg.type() == MessageType.EOF:
            self._handle_upstream_eof(msg)  # type: ignore
            ack()
            return

        # TODO: tirar error si el tipo de msj no se corresponde con el `El`
        # TODO: qué pasa si se cae el filter en el medio? o sea después de
        #       haber redireccionado hacia un par de nodos
        for destination, filter_fn in self.routes:
            filtered = filter_fn.filter(msg)
            destination.send(filtered.serialize())
            logging.debug(f"filtered: {filtered.__dict__}")

        with self._lock:
            self._get_state(msg.client_id).processed_count += 1

        ack()

    def _handle_upstream_eof(self, eof: EOF):
        """Received EOF from upstream (gateway). Starts the ring."""
        with self._lock:
            state = self._get_state(eof.client_id)
            eof.processed_count = state.share_count()
            eof.origin = self.worker_id
            state.is_initiator = True
        logging.info(f"starting ring for client {eof.client_id}")
        self.ring_tx.send(eof.serialize())

    def _handle_ring_eof(self, bytes2: bytes, ack: Callable, nack: Callable):
        """Received ring EOF from the previous worker."""
        eof = EOF.deserialize(bytes2)
        logging.debug(f"received ring eof: {eof.__dict__}")

        ring_done = False
        forward_eof = None

        with self._lock:
            state = self._get_state(eof.client_id)

            if state.done:
                ack()
                return

            pending = state.share_count()
            accumulated = eof.processed_count + pending

            if state.is_initiator and accumulated >= eof.expected_count:
                # message completed the full ring and all counts are in
                ring_done = True
                state.done = True
            else:
                # non-initiator: pass along; or initiator with partial total (race): re-initiate
                origin = self.worker_id if state.is_initiator else eof.origin
                forward_eof = EOF(eof.client_id, accumulated, eof.expected_count, origin)

        if ring_done:
            for destination, _ in self.routes:
                destination.send(EOF(eof.client_id).serialize())
            logging.info(f"ring complete for client {eof.client_id}")
        else:
            self.ring_tx.send(forward_eof.serialize())

        ack()
