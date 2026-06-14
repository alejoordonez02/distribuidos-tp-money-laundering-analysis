import logging
import subprocess
import threading
import time
from typing import Callable, Optional

from .registry import NodeRegistry, Status


def _docker_start(node_id: str) -> None:
    # Recovery (not detection) is allowed to use Docker: start the dead node's
    # container again, reusing its state volume so it restores from checkpoint.
    subprocess.run(
        ["docker", "start", node_id],
        check=False,
        capture_output=True,
        timeout=15,
    )


class Reviver:
    """Revives nodes the registry marked DEAD by starting their container via
    Docker. Detection stays heartbeat-based; only the recovery touches Docker.
    A per-node cooldown avoids hammering a node that is slow to come back."""

    def __init__(
        self,
        registry: NodeRegistry,
        interval: float = 5.0,
        cooldown: float = 15.0,
        start_fn: Optional[Callable[[str], None]] = None,
    ):
        self._registry = registry
        self._interval = interval
        self._cooldown = cooldown
        self._start = start_fn or _docker_start
        self._last_attempt: dict[str, float] = {}

    def run(self, stop: threading.Event) -> None:
        while not stop.is_set():
            self.sweep(time.monotonic())
            stop.wait(self._interval)

    def sweep(self, now: float) -> None:
        nodes, _ = self._registry.snapshot()
        for node in nodes:
            if node.status is not Status.DEAD:
                continue
            if now - self._last_attempt.get(node.node_id, float("-inf")) <= self._cooldown:
                continue
            self._revive(node.node_id, now)

    def _revive(self, node_id: str, now: float) -> None:
        self._last_attempt[node_id] = now
        self._registry.note(node_id, "reviving (docker start)")
        logging.warning("supervisor: reviving %s via docker start", node_id)
        try:
            self._start(node_id)
        except Exception as e:  # never let one failed revive kill the loop
            logging.warning("supervisor: revive of %s failed (%s)", node_id, e)
